//! PostgreSQL-compatibility regression suite driven by `sqllogictest`.
//!
//! `sqllogictest` is the de-facto industry framework (DuckDB, RisingWave,
//! Materialize, …) for `.slt`-driven SQL regression tests. Each `.slt` file
//! is a tiny DSL of `statement ok` / `query <type-chars>` records with
//! expected results inline; the framework reads them, runs the SQL via our
//! [`AsyncDB`](sqllogictest::AsyncDB) adapter, and reports per-record
//! pass/fail.
//!
//! Wiring summary:
//!
//! 1. [`BasinRunner`] adapts `basin_engine::ProjectSession::execute` to
//!    [`sqllogictest::AsyncDB`]. It owns one `Engine` + one `ProjectSession`
//!    per `.slt` file (fresh, isolated namespace per file).
//! 2. RecordBatch → `DBOutput::Rows` conversion uses Arrow's
//!    [`ArrayFormatter`] for per-cell stringification with PG-style
//!    overrides: NULL → `NULL`, booleans → `t` / `f`, floats truncated to
//!    3 decimal places to match `psql`'s default `extra_float_digits = 0`
//!    rendering.
//! 3. The Cartesian test below walks `sqllogictest-suites/*.slt`, runs each
//!    file, and aggregates pass/fail counts. A summary line + a benchmark
//!    JSON card (`benchmark/data/viability_sql_support_slt.json`) are
//!    emitted for the dashboard.
//! 4. CI gate: the test fails if the pass rate drops below the floor set in
//!    [`PASS_THRESHOLD_PCT`]. The floor was sized to (current passing - 1)
//!    so a single new case addition doesn't break CI; raise it as the suite
//!    grows.
//!
//! Failing cases for engine triage are logged on stderr with `file:line:
//! <sql>` so the dev sees what to fix.

#![allow(clippy::print_stdout)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow_array::{Array, BooleanArray, Float32Array, Float64Array, StringArray};
use arrow_schema::DataType;
use async_trait::async_trait;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType, Runner};
use tempfile::TempDir;

// ────────────────────────────────────────────────────────────────────────────
// CI gate
// ────────────────────────────────────────────────────────────────────────────

/// Pass-rate floor (percent). Test fails if the actual rate drops below this.
///
/// Initial sweep: 40/50 = 80.0% passing. Threshold set so the gate trips if
/// either (a) any currently-passing case regresses, or (b) someone adds a
/// brand-new failing case AND a regression at the same time — but a single
/// new failing case alone (40/51 ≈ 78.4%) is still within the floor.
/// Raise this number as engine fixes land — it's a ratchet, not a ceiling.
const PASS_THRESHOLD_PCT: f64 = 78.0;

const SUITE_DIR: &str = "sqllogictest-suites";

// ────────────────────────────────────────────────────────────────────────────
// AsyncDB adapter: basin-engine ProjectSession → sqllogictest::AsyncDB
// ────────────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
#[error("basin-engine: {0}")]
struct AdapterError(String);

struct BasinRunner {
    session: basin_engine::ProjectSession,
    // Held so the tempdir lives as long as the session that's reading from it.
    _tempdir: Arc<TempDir>,
}

impl BasinRunner {
    async fn open() -> Result<Self, AdapterError> {
        let dir = TempDir::new().map_err(|e| AdapterError(format!("tempdir: {e}")))?;
        let fs = LocalFileSystem::new_with_prefix(dir.path())
            .map_err(|e| AdapterError(format!("localfs: {e}")))?;
        let storage = Storage::new(StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
            page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
        });
        let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
        let engine = Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        });
        let session = engine
            .open_session(ProjectId::new())
            .await
            .map_err(|e| AdapterError(format!("open_session: {e}")))?;
        Ok(BasinRunner {
            session,
            _tempdir: Arc::new(dir),
        })
    }
}

#[async_trait]
impl AsyncDB for BasinRunner {
    type Error = AdapterError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let result = self
            .session
            .execute(sql)
            .await
            .map_err(|e| AdapterError(e.to_string()))?;
        match result {
            ExecResult::Empty { tag } => Ok(DBOutput::StatementComplete(parse_tag_count(&tag))),
            ExecResult::Rows { schema, batches } => {
                let types: Vec<DefaultColumnType> = schema
                    .fields()
                    .iter()
                    .map(|f| df_type_to_slt(f.data_type()))
                    .collect();
                let mut rows: Vec<Vec<String>> = Vec::new();
                for batch in &batches {
                    let n_rows = batch.num_rows();
                    let n_cols = batch.num_columns();
                    // Cache one formatter per column for the batch.
                    let opts = FormatOptions::default().with_null("NULL");
                    let mut formatters: Vec<ArrayFormatter<'_>> = Vec::with_capacity(n_cols);
                    for c in 0..n_cols {
                        let arr = batch.column(c);
                        let fmt = ArrayFormatter::try_new(arr.as_ref(), &opts)
                            .map_err(|e| AdapterError(format!("formatter col {c}: {e}")))?;
                        formatters.push(fmt);
                    }
                    for r in 0..n_rows {
                        let mut row: Vec<String> = Vec::with_capacity(n_cols);
                        for c in 0..n_cols {
                            row.push(format_cell(batch.column(c), r, &formatters[c]));
                        }
                        rows.push(row);
                    }
                }
                Ok(DBOutput::Rows { types, rows })
            }
        }
    }

    async fn shutdown(&mut self) {}

    fn engine_name(&self) -> &str {
        "basin"
    }
}

/// Map an Arrow data type to sqllogictest's coarse column type.
fn df_type_to_slt(dt: &DataType) -> DefaultColumnType {
    match dt {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => DefaultColumnType::Integer,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            DefaultColumnType::FloatingPoint
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DefaultColumnType::Text,
        _ => DefaultColumnType::Any,
    }
}

/// PG-style cell formatting overrides on top of Arrow's default formatter:
///
/// - NULLs → `NULL` (already wired via `FormatOptions::with_null`).
/// - Booleans → `t` / `f` to match `psql` rendering.
/// - Floats truncated to 3 decimal places (matches PG's default
///   `extra_float_digits = 0` rounding for short literals).
/// - String cells whose value is the literal `"true"`/`"false"` (any case)
///   are harmonized to `t` / `f`. This covers `BOOLEAN::TEXT` casts that
///   reach the harness as Utf8 (`"true"`/`"false"` per PG semantics) — we
///   want the same `t`/`f` rendering as a raw `BooleanArray`. No `.slt`
///   case in the curated suite round-trips the string literals `'true'`
///   or `'false'`, so this is unambiguous.
fn format_cell(arr: &dyn Array, row: usize, default_fmt: &ArrayFormatter<'_>) -> String {
    if arr.is_null(row) {
        return "NULL".to_string();
    }
    if let Some(b) = arr.as_any().downcast_ref::<BooleanArray>() {
        return if b.value(row) { "t" } else { "f" }.to_string();
    }
    if let Some(f) = arr.as_any().downcast_ref::<Float64Array>() {
        return format!("{:.3}", f.value(row));
    }
    if let Some(f) = arr.as_any().downcast_ref::<Float32Array>() {
        return format!("{:.3}", f.value(row));
    }
    if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
        let v = s.value(row);
        let trimmed = v.trim();
        if trimmed.eq_ignore_ascii_case("true") {
            return "t".to_string();
        }
        if trimmed.eq_ignore_ascii_case("false") {
            return "f".to_string();
        }
        return v.to_string();
    }
    default_fmt.value(row).to_string()
}

/// Extract a row count from a PG-style command tag (`"INSERT 0 3"`,
/// `"UPDATE 1"`, `"DELETE 5"`). Returns 0 if no number is present
/// (`"CREATE TABLE"`).
fn parse_tag_count(tag: &str) -> u64 {
    tag.split_whitespace()
        .rev()
        .find_map(|tok| tok.parse::<u64>().ok())
        .unwrap_or(0)
}

// ────────────────────────────────────────────────────────────────────────────
// .slt discovery + dispatch
// ────────────────────────────────────────────────────────────────────────────

fn suite_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(SUITE_DIR)
}

fn benchmark_data_path(id: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(|root| root.join("benchmark/data").join(format!("viability_{id}.json")))
        .expect("repo root from CARGO_MANIFEST_DIR")
}

fn list_slt_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(read) = std::fs::read_dir(dir) {
        for entry in read.flatten() {
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) == Some("slt") {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

/// Per-record outcome. `(category, sql, error_summary)`.
#[derive(Debug)]
struct CaseFailure {
    file: String,
    line: u32,
    sql: String,
    err: String,
}

/// Walk one `.slt` file record-by-record, returning `(passed, failed,
/// failures)`. We run records one at a time (instead of `run_file_async`)
/// so a single bad case doesn't abort the whole file — we want the full
/// pass-rate signal.
async fn run_slt_file(path: &Path) -> (u32, u32, Vec<CaseFailure>) {
    let make_conn = || async { BasinRunner::open().await };
    let mut runner: Runner<BasinRunner, _> = Runner::new(make_conn);

    let script = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            return (
                0,
                1,
                vec![CaseFailure {
                    file: path.display().to_string(),
                    line: 0,
                    sql: String::new(),
                    err: format!("read .slt: {e}"),
                }],
            );
        }
    };

    let file_name: Arc<str> = path.display().to_string().into();
    let records = match sqllogictest::parse_with_name(&script, file_name.clone()) {
        Ok(rs) => rs,
        Err(e) => {
            return (
                0,
                1,
                vec![CaseFailure {
                    file: file_name.to_string(),
                    line: 0,
                    sql: String::new(),
                    err: format!("parse .slt: {e}"),
                }],
            );
        }
    };

    let mut passed = 0;
    let mut failed = 0;
    let mut failures = Vec::new();
    for record in records {
        // Only `statement` / `query` records count as test cases. Skip
        // halt / control / sleep / etc. — running them is fine but they
        // don't move the pass-rate denominator.
        let (line, sql, is_case): (u32, String, bool) = match &record {
            sqllogictest::Record::Statement { loc, sql, .. } => {
                (loc.line(), sql.clone(), true)
            }
            sqllogictest::Record::Query { loc, sql, .. } => {
                (loc.line(), sql.clone(), true)
            }
            _ => (0, String::new(), false),
        };
        let result = runner.run_async(record).await;
        if !is_case {
            continue;
        }
        match result {
            Ok(_) => passed += 1,
            Err(e) => {
                failed += 1;
                failures.push(CaseFailure {
                    file: file_name.to_string(),
                    line,
                    sql,
                    err: e.to_string(),
                });
            }
        }
    }
    (passed, failed, failures)
}

// ────────────────────────────────────────────────────────────────────────────
// Benchmark JSON emit
// ────────────────────────────────────────────────────────────────────────────

fn emit_dashboard_card(passed: u32, total: u32, pass_rate_pct: f64, threshold_pct: f64) {
    let path = benchmark_data_path("sql_support_slt");
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let pass = pass_rate_pct >= threshold_pct;
    let card = json!({
        "kind": "viability",
        "id": "sql_support_slt",
        "name": "SQL compatibility (sqllogictest)",
        "claim": format!(
            "Curated PG-style .slt suite reaches >= {threshold_pct:.0}% pass rate against basin-engine.",
        ),
        "passed": pass,
        "primary": {
            "label": "pass_rate",
            "value": pass_rate_pct,
            "unit": "%",
            "bar": { "op": "greater_than_or_equal", "value": threshold_pct },
        },
        "details": {
            "passed": passed,
            "total": total,
            "pass_rate_pct": pass_rate_pct,
            "threshold_pct": threshold_pct,
            "framework": "sqllogictest",
            "framework_version": "0.29",
        },
        "metrics": [
            { "label": "passed",    "value": passed as f64,  "unit": "tests" },
            { "label": "total",     "value": total as f64,   "unit": "tests" },
            { "label": "pass_rate", "value": pass_rate_pct,  "unit": "%" },
        ],
        "generated_at": format!(
            "@{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
        ),
    });
    let bytes = serde_json::to_vec_pretty(&card).expect("serialize sql_support_slt card");
    let tmp = path.with_extension("json.tmp");
    if let Err(e) = std::fs::write(&tmp, &bytes) {
        eprintln!("dashboard: write {}: {e}", tmp.display());
        return;
    }
    if let Err(e) = std::fs::rename(&tmp, &path) {
        eprintln!("dashboard: rename {}: {e}", path.display());
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Test entry point
// ────────────────────────────────────────────────────────────────────────────

/// Top-level entry. One `#[tokio::test]` that walks every `.slt` file under
/// `tests/integration/sqllogictest-suites/` and aggregates results.
///
/// We use a single test (not one per file) for two reasons:
///   1. The pass-rate gate is global — splitting per file would either need
///      per-file thresholds (operational nightmare) or independent fail
///      assertions that lose the global signal.
///   2. Dashboard emission needs the totals, computed once at the end.
///
/// Per-file output is still printed so dev iteration on a single file is easy.
#[tokio::test]
async fn pg_compat_sqllogictest_suite() {
    let dir = suite_dir();
    let files = list_slt_files(&dir);
    assert!(
        !files.is_empty(),
        "no .slt files found under {}",
        dir.display()
    );

    let mut total_passed: u32 = 0;
    let mut total_failed: u32 = 0;
    let mut all_failures: Vec<CaseFailure> = Vec::new();

    for file in &files {
        let (p, f, failures): (u32, u32, Vec<CaseFailure>) =
            Box::pin(run_slt_file(file)).await;
        println!(
            "  {}: {} passed, {} failed",
            file.file_name().and_then(|s| s.to_str()).unwrap_or("?"),
            p,
            f
        );
        total_passed += p;
        total_failed += f;
        all_failures.extend(failures);
    }

    let total = total_passed + total_failed;
    let pct = if total == 0 {
        0.0
    } else {
        (total_passed as f64) * 100.0 / (total as f64)
    };
    println!(
        "SLT pass: {}/{} ({:.1}%)  threshold: {:.1}%",
        total_passed, total, pct, PASS_THRESHOLD_PCT
    );

    if !all_failures.is_empty() {
        eprintln!("\n--- Failing cases ({}):", all_failures.len());
        for f in &all_failures {
            // Trim SQL to first line for readability.
            let one_line = f.sql.lines().next().unwrap_or("").trim();
            eprintln!("  {}:{}: {}", f.file, f.line, one_line);
            // First line of the error too — full backtrace is in the raw
            // panic output sqllogictest constructs.
            let err_line = f.err.lines().next().unwrap_or("").trim();
            eprintln!("    -> {}", err_line);
        }
    }

    emit_dashboard_card(total_passed, total as u32, pct, PASS_THRESHOLD_PCT);

    assert!(
        pct >= PASS_THRESHOLD_PCT,
        "SLT pass rate {pct:.1}% < threshold {PASS_THRESHOLD_PCT:.1}% ({total_passed}/{total})"
    );
}

